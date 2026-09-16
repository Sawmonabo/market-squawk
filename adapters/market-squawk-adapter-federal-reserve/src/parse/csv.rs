use std::collections::{BTreeMap, BTreeSet};

use csv::{ReaderBuilder, StringRecord};
use rust_decimal::Decimal;
use sha2::{Digest as _, Sha256};

use crate::contract::{BoardArtifactKind, BoardFileFormat, BoardSeriesContract};
use crate::digest::{finish, sha256, update_tag, update_u64};
use crate::model::{BoardArtifactReceipt, BoardObservation, BoardPeriod, BoardSeries, BoardValue};
use crate::{BoardAdapterError, BoardDatasetContract, BoardParseLimits, ParsedBoardDataset};

const HEADER_LABELS: [&str; 6] = [
    "Series Description",
    "Unit:",
    "Multiplier:",
    "Currency:",
    "Unique Identifier: ",
    "Time Period",
];

/// Parses one exact-label DDP series-column CSV response.
pub fn parse_csv(
    contract: &BoardDatasetContract,
    bytes: &[u8],
    limits: BoardParseLimits,
) -> Result<ParsedBoardDataset, BoardAdapterError> {
    parse_csv_inner(contract, bytes, limits, None)
}

pub(crate) fn parse_csv_controlled(
    contract: &BoardDatasetContract,
    bytes: &[u8],
    limits: BoardParseLimits,
    control: &dyn market_squawk_platform::ResearchObjectControl,
) -> Result<ParsedBoardDataset, BoardAdapterError> {
    parse_csv_inner(contract, bytes, limits, Some(control))
}

fn parse_csv_inner(
    contract: &BoardDatasetContract,
    bytes: &[u8],
    limits: BoardParseLimits,
    control: Option<&dyn market_squawk_platform::ResearchObjectControl>,
) -> Result<ParsedBoardDataset, BoardAdapterError> {
    let checkpoint = || -> Result<(), BoardAdapterError> {
        if let Some(control) = control {
            control.checkpoint(
                market_squawk_platform::ResearchObjectControlPoint::BeforeVerification,
            )?;
        }
        Ok(())
    };
    checkpoint()?;
    if contract.format() != BoardFileFormat::DdpCsvSeriesColumnV1 {
        return Err(BoardAdapterError::FormatMismatch);
    }
    if bytes.is_empty() || bytes.len() > limits.max_source_bytes() {
        return Err(BoardAdapterError::ByteLimitExceeded);
    }
    let expected = contract
        .series_scope()
        .exact_series()
        .ok_or(BoardAdapterError::InvalidContract)?;
    if expected.len() > limits.max_series() {
        return Err(BoardAdapterError::StructuralLimitExceeded);
    }
    let mut reader = ReaderBuilder::new()
        .has_headers(false)
        .flexible(false)
        .from_reader(bytes);
    let mut records = reader.records();
    let mut metadata = Vec::new();
    metadata
        .try_reserve_exact(HEADER_LABELS.len())
        .map_err(|_| BoardAdapterError::AllocationFailed)?;
    for label in HEADER_LABELS {
        checkpoint()?;
        let record = records
            .next()
            .ok_or(BoardAdapterError::CsvSchemaDrift)?
            .map_err(|error| BoardAdapterError::InvalidCsv(error.to_string()))?;
        require_width_and_label(&record, expected.len(), label)?;
        metadata.push(record);
    }
    validate_metadata(&metadata, expected)?;

    let mut columns = vec![Vec::new(); expected.len()];
    let mut periods = BTreeSet::new();
    let mut observation_count = 0_usize;
    for result in records {
        checkpoint()?;
        let record = result.map_err(|error| BoardAdapterError::InvalidCsv(error.to_string()))?;
        if record.len() != expected.len() + 1 {
            return Err(BoardAdapterError::CsvSchemaDrift);
        }
        let period_text = record.get(0).ok_or(BoardAdapterError::CsvSchemaDrift)?;
        let period = BoardPeriod::parse(period_text, contract.frequency())?;
        if !periods.insert(period.clone()) {
            return Err(BoardAdapterError::DuplicateIdentity);
        }
        observation_count = observation_count
            .checked_add(expected.len())
            .ok_or(BoardAdapterError::CountOverflow)?;
        if observation_count > limits.max_observations() {
            return Err(BoardAdapterError::StructuralLimitExceeded);
        }
        for (index, column) in columns.iter_mut().enumerate() {
            let raw = record
                .get(index + 1)
                .ok_or(BoardAdapterError::CsvSchemaDrift)?;
            let value = BoardValue::parse(Some(raw), if raw == "ND" { "ND" } else { "A" })?;
            column.push(BoardObservation::try_new(
                period.clone(),
                value,
                BTreeMap::new(),
            )?);
        }
    }
    if periods.is_empty() {
        return Err(BoardAdapterError::CsvSchemaDrift);
    }

    let descriptions = metadata.first().ok_or(BoardAdapterError::CsvSchemaDrift)?;
    let mut parsed_series = Vec::new();
    parsed_series
        .try_reserve_exact(expected.len())
        .map_err(|_| BoardAdapterError::AllocationFailed)?;
    for (index, (series_contract, observations)) in expected.iter().zip(columns).enumerate() {
        checkpoint()?;
        let description = descriptions
            .get(index + 1)
            .ok_or(BoardAdapterError::CsvSchemaDrift)?;
        parsed_series.push(build_series(series_contract, description, observations)?);
    }

    let payload_digest = sha256(bytes);
    let schema_digest = csv_schema_digest(&metadata, contract.frequency());
    let artifact = BoardArtifactReceipt::new(
        "response.csv",
        BoardArtifactKind::DataCsv,
        bytes.len(),
        payload_digest,
    )?;
    checkpoint()?;
    ParsedBoardDataset::try_new(
        contract,
        &contract.request(),
        payload_digest,
        schema_digest,
        None,
        vec![artifact],
        parsed_series,
    )
}

/// A private subset, never a complete original or publication authority. Global ordinals are
/// retained separately because omitted series/observations must not renumber native row maps.
pub(crate) struct SelectedBoardCsv {
    pub(crate) parsed: ParsedBoardDataset,
    pub(crate) global_ordinals: Vec<u64>,
}

/// The caller physically authenticates the original file and its committed transport receipt.
/// Scan only period coordinates, then seek exact CSV record boundaries for selected *complete*
/// partitions. No unselected value is normalized and no whole-history typed parse is retained.
#[allow(clippy::too_many_arguments)]
pub(crate) fn parse_csv_selected_partitions(
    contract: &BoardDatasetContract,
    bytes: &[u8],
    limits: BoardParseLimits,
    original_observation_count: u64,
    selected_series: &str,
    dates: &[market_squawk_domain::CalendarDate; 11],
    rows_per_partition: u32,
    control: &dyn market_squawk_platform::ResearchObjectControl,
) -> Result<SelectedBoardCsv, BoardAdapterError> {
    let checkpoint = || {
        control
            .checkpoint(market_squawk_platform::ResearchObjectControlPoint::BeforeVerification)
            .map_err(BoardAdapterError::from)
    };
    checkpoint()?;
    if contract.format() != BoardFileFormat::DdpCsvSeriesColumnV1
        || dates.windows(2).any(|pair| pair[0] >= pair[1])
        || rows_per_partition == 0
    {
        return Err(BoardAdapterError::InvalidContract);
    }
    if bytes.is_empty() || bytes.len() > limits.max_source_bytes() {
        return Err(BoardAdapterError::ByteLimitExceeded);
    }
    let expected = contract
        .series_scope()
        .exact_series()
        .ok_or(BoardAdapterError::InvalidContract)?;
    if expected.len() != 11
        || expected.len() > limits.max_series()
        || original_observation_count == 0
        || original_observation_count > limits.max_observations() as u64
        || original_observation_count % expected.len() as u64 != 0
    {
        return Err(BoardAdapterError::StructuralLimitExceeded);
    }
    let selected_series_index = expected
        .iter()
        .position(|series| series.unique_id() == selected_series)
        .ok_or(BoardAdapterError::SeriesMismatch)?;
    let row_count = original_observation_count / expected.len() as u64;
    let mut reader = ReaderBuilder::new()
        .has_headers(false)
        .flexible(false)
        .from_reader(std::io::Cursor::new(bytes));
    let mut metadata = Vec::with_capacity(HEADER_LABELS.len());
    let mut record = StringRecord::new();
    for label in HEADER_LABELS {
        checkpoint()?;
        if !reader
            .read_record(&mut record)
            .map_err(|error| BoardAdapterError::InvalidCsv(error.to_string()))?
        {
            return Err(BoardAdapterError::CsvSchemaDrift);
        }
        require_width_and_label(&record, expected.len(), label)?;
        metadata.push(record.clone());
    }
    validate_metadata(&metadata, expected)?;
    let mut offsets = Vec::new();
    offsets
        .try_reserve_exact(row_count as usize)
        .map_err(|_| BoardAdapterError::AllocationFailed)?;
    let mut selected_rows = [None; 11];
    let mut previous_period = None;
    loop {
        checkpoint()?;
        if !reader
            .read_record(&mut record)
            .map_err(|error| BoardAdapterError::InvalidCsv(error.to_string()))?
        {
            break;
        }
        if record.len() != expected.len() + 1 || offsets.len() as u64 >= row_count {
            return Err(BoardAdapterError::CsvSchemaDrift);
        }
        let period = BoardPeriod::parse(
            record.get(0).ok_or(BoardAdapterError::CsvSchemaDrift)?,
            contract.frequency(),
        )?;
        if previous_period
            .as_ref()
            .is_some_and(|previous| previous >= &period)
        {
            return Err(BoardAdapterError::DuplicateIdentity);
        }
        let crate::BoardPeriodValue::CalendarDate { date } = period.value() else {
            return Err(BoardAdapterError::FormatMismatch);
        };
        if let Ok(index) = dates.binary_search(date) {
            selected_rows[index] = Some(offsets.len() as u64);
        }
        offsets.push(
            record
                .position()
                .ok_or(BoardAdapterError::CsvSchemaDrift)?
                .byte(),
        );
        previous_period = Some(period);
    }
    if offsets.len() as u64 != row_count || selected_rows.iter().any(Option::is_none) {
        return Err(BoardAdapterError::SeriesMismatch);
    }
    let partition_size = u64::from(rows_per_partition);
    let mut partitions = BTreeSet::new();
    for row in selected_rows {
        let ordinal = selected_series_index as u64 * row_count
            + row.ok_or(BoardAdapterError::SeriesMismatch)?;
        partitions.insert(ordinal / partition_size);
    }
    let mut columns = vec![Vec::new(); expected.len()];
    let mut global_ordinals = Vec::new();
    global_ordinals
        .try_reserve_exact(partitions.len() * rows_per_partition as usize)
        .map_err(|_| BoardAdapterError::AllocationFailed)?;
    for partition in partitions {
        let first = partition * partition_size;
        let end = (first + partition_size).min(original_observation_count);
        for ordinal in first..end {
            checkpoint()?;
            let series_index = (ordinal / row_count) as usize;
            let observation_index = (ordinal % row_count) as usize;
            let mut position = csv::Position::new();
            position.set_byte(offsets[observation_index]);
            reader
                .seek(position)
                .map_err(|error| BoardAdapterError::InvalidCsv(error.to_string()))?;
            if !reader
                .read_record(&mut record)
                .map_err(|error| BoardAdapterError::InvalidCsv(error.to_string()))?
                || record.len() != expected.len() + 1
            {
                return Err(BoardAdapterError::CsvSchemaDrift);
            }
            let period = BoardPeriod::parse(
                record.get(0).ok_or(BoardAdapterError::CsvSchemaDrift)?,
                contract.frequency(),
            )?;
            let raw = record
                .get(series_index + 1)
                .ok_or(BoardAdapterError::CsvSchemaDrift)?;
            let value = BoardValue::parse(Some(raw), if raw == "ND" { "ND" } else { "A" })?;
            columns[series_index].push(BoardObservation::try_new(period, value, BTreeMap::new())?);
            global_ordinals.push(ordinal);
        }
    }
    let mut series = Vec::new();
    for (index, (contract, observations)) in expected.iter().zip(columns).enumerate() {
        checkpoint()?;
        if !observations.is_empty() {
            series.push(build_series(
                contract,
                field(&metadata, 0, index + 1)?,
                observations,
            )?);
        }
    }
    let payload_digest = sha256(bytes);
    let artifact = BoardArtifactReceipt::new(
        "response.csv",
        BoardArtifactKind::DataCsv,
        bytes.len(),
        payload_digest,
    )?;
    let parsed = ParsedBoardDataset::try_new(
        contract,
        &contract.request(),
        payload_digest,
        csv_schema_digest(&metadata, contract.frequency()),
        None,
        vec![artifact],
        series,
    )?;
    checkpoint()?;
    Ok(SelectedBoardCsv {
        parsed,
        global_ordinals,
    })
}

fn require_width_and_label(
    record: &StringRecord,
    series_count: usize,
    label: &str,
) -> Result<(), BoardAdapterError> {
    if record.len() != series_count + 1 || record.get(0) != Some(label) {
        Err(BoardAdapterError::CsvSchemaDrift)
    } else {
        Ok(())
    }
}

fn validate_metadata(
    metadata: &[StringRecord],
    expected: &[BoardSeriesContract],
) -> Result<(), BoardAdapterError> {
    for (index, item) in expected.iter().enumerate() {
        let column = index + 1;
        let description = field(metadata, 0, column)?;
        if description.is_empty()
            || description.len() > 8 * 1024
            || item
                .expected_description()
                .is_some_and(|value| value != description)
            || field(metadata, 1, column)? != item.unit()
            || Decimal::from_str_exact(field(metadata, 2, column)?)
                .map_err(|_| BoardAdapterError::CsvSchemaDrift)?
                .normalize()
                != item.multiplier()
            || field(metadata, 3, column)? != item.currency()
            || field(metadata, 4, column)? != item.unique_id()
            || field(metadata, 5, column)? != item.series_name()
        {
            return Err(BoardAdapterError::SeriesMismatch);
        }
    }
    Ok(())
}

fn field(records: &[StringRecord], row: usize, column: usize) -> Result<&str, BoardAdapterError> {
    records
        .get(row)
        .and_then(|record| record.get(column))
        .ok_or(BoardAdapterError::CsvSchemaDrift)
}

fn build_series(
    contract: &BoardSeriesContract,
    description: &str,
    observations: Vec<BoardObservation>,
) -> Result<BoardSeries, BoardAdapterError> {
    BoardSeries::try_new(
        contract.unique_id().into(),
        contract.series_name().into(),
        description.into(),
        contract.unit().into(),
        contract.multiplier(),
        contract.currency().into(),
        contract.frequency(),
        contract.lifecycle().clone(),
        BTreeMap::new(),
        observations,
    )
}

fn csv_schema_digest(metadata: &[StringRecord], frequency: crate::BoardFrequency) -> [u8; 32] {
    let mut digest = Sha256::new();
    update_tag(
        &mut digest,
        "market-squawk-federal-reserve-ddp-series-column-schema-v1",
    );
    update_tag(&mut digest, frequency.as_str());
    update_u64(&mut digest, metadata.len() as u64);
    for row in metadata {
        update_u64(&mut digest, row.len() as u64);
        for field in row {
            update_tag(&mut digest, field);
        }
    }
    finish(digest)
}
