use std::str::FromStr;

use market_squawk_domain::CalendarDate;
use rust_decimal::Decimal;
use serde::Deserialize;
use thiserror::Error;

/// Conservative parser admission limits for a FRED JSON page.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct FredParseLimits {
    pub(crate) max_records: usize,
    pub(crate) max_bytes: usize,
    pub(crate) max_string_bytes: usize,
}

impl FredParseLimits {
    /// Builds explicit non-zero parser limits.
    pub fn try_new(
        max_records: usize,
        max_bytes: usize,
        max_string_bytes: usize,
    ) -> Result<Self, FredProtocolError> {
        if max_records == 0
            || max_bytes == 0
            || max_string_bytes == 0
            || max_string_bytes > max_bytes
        {
            return Err(FredProtocolError::InvalidLimit);
        }
        Ok(Self {
            max_records,
            max_bytes,
            max_string_bytes,
        })
    }

    /// Returns limits suitable for the documented maximum observation page.
    pub const fn production_defaults() -> Self {
        Self {
            max_records: 100_000,
            max_bytes: 32 * 1024 * 1024,
            max_string_bytes: 8 * 1024,
        }
    }
}

/// A bounded protocol or schema failure while decoding FRED data.
#[derive(Debug, Error)]
pub enum FredProtocolError {
    /// A configured parser limit is invalid.
    #[error("FRED parser limits must be non-zero and internally consistent")]
    InvalidLimit,
    /// The response body exceeds the configured byte budget.
    #[error("FRED response body exceeds the configured byte budget")]
    BodyTooLarge,
    /// The response is not the supported documented JSON shape.
    #[error("invalid FRED JSON response: {0}")]
    InvalidJson(#[from] serde_json::Error),
    /// A response field violates the supported protocol contract.
    #[error("invalid FRED protocol field: {0}")]
    InvalidField(&'static str),
    /// A provider string exceeds the configured retained-size bound.
    #[error("FRED provider string exceeds the configured retained-size bound")]
    StringTooLarge,
    /// A decimal value is neither exact nor the provider missing marker.
    #[error("invalid FRED observation value")]
    InvalidValue,
}

/// One FRED observation with its exact civil-date revision interval.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FredObservation {
    realtime_start: CalendarDate,
    realtime_end: CalendarDate,
    observation_date: CalendarDate,
    raw_value: String,
    value: Option<Decimal>,
}

impl FredObservation {
    /// Returns the first civil date on which this revision is applicable.
    pub const fn realtime_start(&self) -> CalendarDate {
        self.realtime_start
    }

    /// Returns the inclusive final civil date for this revision.
    pub const fn realtime_end(&self) -> CalendarDate {
        self.realtime_end
    }

    /// Returns the observation reference date without inventing a time zone.
    pub const fn observation_date(&self) -> CalendarDate {
        self.observation_date
    }

    /// Returns the provider's exact lexical value.
    pub fn raw_value(&self) -> &str {
        &self.raw_value
    }

    /// Returns the exact decimal value, or `None` for the provider's `.` marker.
    pub const fn value(&self) -> Option<Decimal> {
        self.value
    }
}

/// One validated, cursor-bearing FRED observation page.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FredObservationPage {
    realtime_start: CalendarDate,
    realtime_end: CalendarDate,
    observation_start: CalendarDate,
    observation_end: CalendarDate,
    units: String,
    count: usize,
    offset: usize,
    limit: usize,
    next_offset: Option<usize>,
    observations: Vec<FredObservation>,
}

impl FredObservationPage {
    /// Parses the documented JSON `output_type=1` observation representation.
    pub fn parse(bytes: &[u8], limits: FredParseLimits) -> Result<Self, FredProtocolError> {
        admit_body(bytes, limits)?;
        let wire: ObservationPageWire = serde_json::from_slice(bytes)?;
        validate_strings(
            [
                wire.realtime_start.as_str(),
                wire.realtime_end.as_str(),
                wire.observation_start.as_str(),
                wire.observation_end.as_str(),
                wire.units.as_str(),
                wire.file_type.as_str(),
                wire.order_by.as_str(),
                wire.sort_order.as_str(),
            ],
            limits,
        )?;
        if wire.output_type != 1
            || wire.file_type != "json"
            || wire.order_by != "observation_date"
            || wire.sort_order != "asc"
        {
            return Err(FredProtocolError::InvalidField("response mode"));
        }
        validate_page(
            wire.count,
            wire.offset,
            wire.limit,
            wire.observations.len(),
            limits,
        )?;

        let realtime_start = parse_date(&wire.realtime_start)?;
        let realtime_end = parse_date(&wire.realtime_end)?;
        let observation_start = parse_date(&wire.observation_start)?;
        let observation_end = parse_date(&wire.observation_end)?;
        if realtime_start > realtime_end || observation_start > observation_end {
            return Err(FredProtocolError::InvalidField("realtime interval"));
        }

        let mut observations = Vec::with_capacity(wire.observations.len());
        let mut previous_observation_date = None;
        let mut previous_realtime_start = None;
        for observation in wire.observations {
            validate_strings(
                [
                    observation.realtime_start.as_str(),
                    observation.realtime_end.as_str(),
                    observation.date.as_str(),
                    observation.value.as_str(),
                ],
                limits,
            )?;
            let row_start = parse_date(&observation.realtime_start)?;
            let row_end = parse_date(&observation.realtime_end)?;
            let observation_date = parse_date(&observation.date)?;
            if row_start > row_end {
                return Err(FredProtocolError::InvalidField(
                    "observation realtime interval",
                ));
            }
            if previous_observation_date.is_some_and(|previous| observation_date < previous)
                || (previous_observation_date == Some(observation_date)
                    && previous_realtime_start.is_some_and(|previous| row_start <= previous))
            {
                return Err(FredProtocolError::InvalidField("observation ordering"));
            }
            previous_observation_date = Some(observation_date);
            previous_realtime_start = Some(row_start);
            let value = if observation.value == "." {
                None
            } else {
                Some(
                    Decimal::from_str_exact(&observation.value)
                        .map_err(|_| FredProtocolError::InvalidValue)?,
                )
            };
            observations.push(FredObservation {
                realtime_start: row_start,
                realtime_end: row_end,
                observation_date,
                raw_value: observation.value,
                value,
            });
        }

        let consumed = wire
            .offset
            .checked_add(observations.len())
            .ok_or(FredProtocolError::InvalidField("page cursor"))?;
        Ok(Self {
            realtime_start,
            realtime_end,
            observation_start,
            observation_end,
            units: wire.units,
            count: wire.count,
            offset: wire.offset,
            limit: wire.limit,
            next_offset: (consumed < wire.count).then_some(consumed),
            observations,
        })
    }

    /// Returns the provider's total matching observation count.
    pub const fn count(&self) -> usize {
        self.count
    }

    /// Returns this page's zero-based offset.
    pub const fn offset(&self) -> usize {
        self.offset
    }

    /// Returns the provider-declared page limit.
    pub const fn limit(&self) -> usize {
        self.limit
    }

    /// Returns the next required offset, or `None` when the page is terminal.
    pub const fn next_offset(&self) -> Option<usize> {
        self.next_offset
    }

    /// Returns the validated observations.
    pub fn observations(&self) -> &[FredObservation] {
        &self.observations
    }

    /// Returns the page-level closed realtime interval start.
    pub const fn realtime_start(&self) -> CalendarDate {
        self.realtime_start
    }

    /// Returns the page-level closed realtime interval end.
    pub const fn realtime_end(&self) -> CalendarDate {
        self.realtime_end
    }

    /// Returns the provider-declared first observation civil date for this response.
    pub const fn observation_start(&self) -> CalendarDate {
        self.observation_start
    }

    /// Returns the provider-declared final observation civil date for this response.
    pub const fn observation_end(&self) -> CalendarDate {
        self.observation_end
    }

    /// Returns the exact provider unit mode declared by the observation response.
    pub fn units(&self) -> &str {
        &self.units
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ObservationPageWire {
    realtime_start: String,
    realtime_end: String,
    observation_start: String,
    observation_end: String,
    units: String,
    output_type: u8,
    file_type: String,
    order_by: String,
    sort_order: String,
    count: usize,
    offset: usize,
    limit: usize,
    observations: Vec<ObservationWire>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ObservationWire {
    realtime_start: String,
    realtime_end: String,
    date: String,
    value: String,
}

pub(crate) fn admit_body(bytes: &[u8], limits: FredParseLimits) -> Result<(), FredProtocolError> {
    if bytes.len() > limits.max_bytes {
        return Err(FredProtocolError::BodyTooLarge);
    }
    Ok(())
}

pub(crate) fn validate_page(
    count: usize,
    offset: usize,
    limit: usize,
    returned: usize,
    limits: FredParseLimits,
) -> Result<(), FredProtocolError> {
    if limit == 0 || returned > limit || returned > limits.max_records {
        return Err(FredProtocolError::InvalidField("page size"));
    }
    let consumed = offset
        .checked_add(returned)
        .ok_or(FredProtocolError::InvalidField("page cursor"))?;
    if offset > count || consumed > count || (returned == 0 && offset < count) {
        return Err(FredProtocolError::InvalidField("page cursor"));
    }
    Ok(())
}

pub(crate) fn validate_strings<'a>(
    values: impl IntoIterator<Item = &'a str>,
    limits: FredParseLimits,
) -> Result<(), FredProtocolError> {
    if values
        .into_iter()
        .any(|value| value.len() > limits.max_string_bytes)
    {
        return Err(FredProtocolError::StringTooLarge);
    }
    Ok(())
}

pub(crate) fn parse_date(value: &str) -> Result<CalendarDate, FredProtocolError> {
    let bytes = value.as_bytes();
    if bytes.len() != 10
        || bytes.get(4) != Some(&b'-')
        || bytes.get(7) != Some(&b'-')
        || !bytes[0..4].iter().all(u8::is_ascii_digit)
        || !bytes[5..7].iter().all(u8::is_ascii_digit)
        || !bytes[8..10].iter().all(u8::is_ascii_digit)
    {
        return Err(FredProtocolError::InvalidField("calendar date"));
    }
    let year = u16::from_str(&value[0..4])
        .map_err(|_| FredProtocolError::InvalidField("calendar date"))?;
    let month =
        u8::from_str(&value[5..7]).map_err(|_| FredProtocolError::InvalidField("calendar date"))?;
    let day = u8::from_str(&value[8..10])
        .map_err(|_| FredProtocolError::InvalidField("calendar date"))?;
    CalendarDate::new(year, month, day)
        .map_err(|_| FredProtocolError::InvalidField("calendar date"))
}
