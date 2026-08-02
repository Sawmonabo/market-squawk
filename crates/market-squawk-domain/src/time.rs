use std::fmt;

use serde::{Deserialize, Serialize};

/// A checked timestamp conversion or arithmetic failure.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TimeError {
    /// Signed Unix-nanosecond arithmetic exceeded `i64`.
    Overflow,
    /// A civil calendar date had an invalid year, month, or day.
    InvalidCalendarDate,
}

impl fmt::Display for TimeError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Overflow => formatter.write_str("timestamp arithmetic overflow"),
            Self::InvalidCalendarDate => formatter.write_str("invalid Gregorian calendar date"),
        }
    }
}

/// A proleptic Gregorian civil date without an invented time of day or time zone.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
pub struct CalendarDate {
    year: u16,
    month: u8,
    day: u8,
}

impl CalendarDate {
    /// Constructs a valid calendar date.
    ///
    /// # Errors
    ///
    /// Rejects year zero, months outside 1 through 12, and days outside the selected month.
    pub const fn new(year: u16, month: u8, day: u8) -> Result<Self, TimeError> {
        let days = match month {
            1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
            4 | 6 | 9 | 11 => 30,
            2 if is_leap_year(year) => 29,
            2 => 28,
            _ => return Err(TimeError::InvalidCalendarDate),
        };
        if year == 0 || day == 0 || day > days {
            Err(TimeError::InvalidCalendarDate)
        } else {
            Ok(Self { year, month, day })
        }
    }

    /// Returns the Gregorian year.
    pub const fn year(self) -> u16 {
        self.year
    }

    /// Returns the month from 1 through 12.
    pub const fn month(self) -> u8 {
        self.month
    }

    /// Returns the day of month.
    pub const fn day(self) -> u8 {
        self.day
    }

    /// Returns the Arrow-compatible signed day offset from 1970-01-01.
    ///
    /// This conversion preserves calendar precision: it does not assign a time of day or time
    /// zone to the date.
    pub const fn days_since_unix_epoch(self) -> i32 {
        let month = self.month as i32;
        let year = self.year as i32 - if month <= 2 { 1 } else { 0 };
        let era = year / 400;
        let year_of_era = year - (era * 400);
        let month_prime = month + if month > 2 { -3 } else { 9 };
        let day_of_year = ((153 * month_prime + 2) / 5) + self.day as i32 - 1;
        let day_of_era =
            (year_of_era * 365) + (year_of_era / 4) - (year_of_era / 100) + day_of_year;
        (era * 146_097) + day_of_era - 719_468
    }
}

const fn is_leap_year(year: u16) -> bool {
    year.is_multiple_of(4) && (!year.is_multiple_of(100) || year.is_multiple_of(400))
}

impl fmt::Display for CalendarDate {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "{:04}-{:02}-{:02}",
            self.year, self.month, self.day
        )
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct CalendarDateWire {
    year: u16,
    month: u8,
    day: u8,
}

impl<'de> Deserialize<'de> for CalendarDate {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let wire = CalendarDateWire::deserialize(deserializer)?;
        Self::new(wire.year, wire.month, wire.day).map_err(serde::de::Error::custom)
    }
}

impl std::error::Error for TimeError {}

/// A UTC instant represented as signed nanoseconds from the Unix epoch.
///
/// This stable scalar contract contains no wall-clock access or I/O. Its inclusive representation
/// boundaries are `i64::MIN` through `i64::MAX` Unix nanoseconds; adapters must perform checked
/// conversion before constructing it. Qualification builds ordering-rich provenance on this type.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(transparent)]
pub struct Timestamp(i64);

impl Timestamp {
    /// Constructs a timestamp from an already range-checked signed Unix-nanosecond value.
    pub const fn from_unix_nanos(value: i64) -> Self {
        Self(value)
    }

    /// Returns signed Unix nanoseconds.
    pub const fn unix_nanos(self) -> i64 {
        self.0
    }

    /// Adds a signed nanosecond offset using checked arithmetic.
    ///
    /// # Errors
    ///
    /// Returns [`TimeError::Overflow`] outside the signed `i64` representation.
    pub fn checked_add_nanos(self, offset: i64) -> Result<Self, TimeError> {
        self.0
            .checked_add(offset)
            .map(Self)
            .ok_or(TimeError::Overflow)
    }

    /// Subtracts a signed nanosecond offset using checked arithmetic.
    ///
    /// # Errors
    ///
    /// Returns [`TimeError::Overflow`] outside the signed `i64` representation.
    pub fn checked_sub_nanos(self, offset: i64) -> Result<Self, TimeError> {
        self.0
            .checked_sub(offset)
            .map(Self)
            .ok_or(TimeError::Overflow)
    }
}
